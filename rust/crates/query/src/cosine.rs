use std::sync::Arc;

use arrow_array::{Array, ArrowPrimitiveType, FixedSizeListArray, Float64Array, ListArray, PrimitiveArray, UnionArray, builder::Float64Builder, types::{Float32Type, Float64Type}};
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

pub(crate) fn parameter(expr: &Expr) -> Option<usize> {
    match inputs(expr) {
        Some((_, Expr::Parameter { index, .. })) => Some(*index),
        _ => None,
    }
}

pub(crate) fn in_projection(relation: &rtdb_plan::Relation) -> bool {
    use rtdb_plan::RelationNode;
    match relation.node() {
        RelationNode::Project { input, expressions } | RelationNode::Rename { input, expressions, .. } =>
            expressions.iter().any(|named| shaped(&named.expr)) || in_projection(input),
        RelationNode::SortLimit { input, .. } | RelationNode::Filter { input, .. } => in_projection(input),
        _ => false,
    }
}

pub(crate) fn packed_parameters(relation: &rtdb_plan::Relation, width: usize) -> Vec<bool> {
    use rtdb_plan::RelationNode;
    fn visit(relation: &rtdb_plan::Relation, used: &mut [bool], scalar: &mut [bool]) -> bool {
        let projection = matches!(relation.node(), RelationNode::Project { .. } | RelationNode::Rename { .. });
        match relation.node() {
            RelationNode::Scan(scan) if scan.matches.is_empty() && scan.ordered_top_k.is_none() && scan.top_k.is_none() => (),
            RelationNode::Project { .. } | RelationNode::Rename { .. } | RelationNode::Filter { .. } | RelationNode::SortLimit { .. } => (),
            _ => return false,
        }
        for expression in relation.expressions() {
            if projection && let Some(index) = parameter(expression) {
                if let Some(packed) = used.get_mut(index) { *packed = true; }
            } else {
                expression.visit(&mut |expr| if let Expr::Parameter { index, .. } = expr
                    && let Some(required) = scalar.get_mut(*index) { *required = true; });
            }
        }
        relation.children().into_iter().all(|child| visit(child, used, scalar))
    }
    let mut used = vec![false; width];
    let mut scalar = vec![false; width];
    if !visit(relation, &mut used, &mut scalar) { used.fill(false); }
    used.iter_mut().zip(scalar).for_each(|(candidate, scalar)| *candidate &= !scalar);
    used
}

pub(crate) fn fixed_float(array: &dyn Array) -> bool {
    matches!(array.data_type(), DataType::FixedSizeList(item, width)
        if *width > 0 && item.metadata().is_empty() && matches!(item.data_type(), DataType::Float32 | DataType::Float64))
}

pub(crate) struct Projection {
    position: usize,
    query: CosineVector,
    _memory: Arc<MemoryReservation>,
}

impl Projection {
    pub(crate) fn bind(expr: &Expr, schema: &Schema, parameters: &crate::parameters::BoundParameters, memory: &Arc<MemoryBudget>) -> Result<Option<Self>> {
        let Some((id, other)) = inputs(expr) else { return Ok(None) };
        let Some(position) = schema.position(id) else { return Ok(None) };
        let prepared = match other {
            Expr::Literal(value) => Self::prepare_value(value, memory),
            Expr::Parameter { index, .. } => match parameters.vector(*index) {
                Some(array) => Self::prepare_arrow(array, memory),
                None => match parameters.values().get(*index) { Some(value) => Self::prepare_value(value, memory), None => Ok(None) },
            },
            Expr::List(items) => Self::prepare(items.iter().filter_map(|item| match item { Expr::Literal(value) => Some(value), _ => None }), memory),
            _ => return Ok(None),
        }?;
        Ok(prepared.map(|(query, _memory)| Self { position, query, _memory }))
    }

    fn prepare_value(value: &Value, memory: &Arc<MemoryBudget>) -> Result<Option<(CosineVector, Arc<MemoryReservation>)>> {
        let Some(values) = value.list_view() else { return Ok(None); };
        let bytes = values.len().checked_mul(8).and_then(|bytes| bytes.checked_add(256))
            .ok_or_else(|| crate::invalid("vector-size", "Prepared vector exceeds addressable memory"))?;
        let Ok(reservation) = memory.reserve(bytes) else { return Ok(None); };
        Ok(CosineVector::prepare_value(value).ok().map(|query| (query, reservation)))
    }

    fn prepare<'a>(values: impl Iterator<Item = &'a Value> + Clone, memory: &Arc<MemoryBudget>) -> Result<Option<(CosineVector, Arc<MemoryReservation>)>> {
        let len = values.clone().count();
        if len == 0 { return Ok(None); }
        let bytes = len.checked_mul(8).and_then(|bytes| bytes.checked_add(256))
            .ok_or_else(|| crate::invalid("vector-size", "Prepared vector exceeds addressable memory"))?;
        let Ok(reservation) = memory.reserve(bytes) else { return Ok(None) };
        Ok(CosineVector::prepare(values, len).ok().map(|query| (query, reservation)))
    }

    fn prepare_arrow(array: &FixedSizeListArray, memory: &Arc<MemoryBudget>) -> Result<Option<(CosineVector, Arc<MemoryReservation>)>> {
        if array.len() != 1 || array.is_null(0) { return Ok(None); }
        let child = array.value(0);
        if child.null_count() != 0 { return Ok(None); }
        let bytes = child.len().checked_mul(8).and_then(|bytes| bytes.checked_add(256))
            .ok_or_else(|| crate::invalid("vector-size", "Prepared vector exceeds addressable memory"))?;
        let Ok(reservation) = memory.reserve(bytes) else { return Ok(None) };
        let prepare = |values: &arrow_array::Float64Array| CosineVector::prepare_components(values.values().iter().copied().map(Ok), values.len());
        let query = if let Some(values) = child.as_any().downcast_ref::<arrow_array::Float64Array>() {
            prepare(values)
        } else if let Some(values) = child.as_any().downcast_ref::<arrow_array::Float32Array>() {
            CosineVector::prepare_components(values.values().iter().copied().map(|value| Ok(f64::from(value))), values.len())
        } else { return Ok(None); };
        Ok(query.ok().map(|query| (query, reservation)))
    }

    pub(crate) fn column(&self, batch: &ColumnBatch) -> Option<Column> {
        let Column::Arrow { array, logical: Some(logical), .. } = batch.columns.get(self.position)? else { return None };
        if !crate::columnar::has_float_vector(logical) { return None; }
        let width = self.query.dimensions();
        let (children, rows): (&dyn Array, _) = if let Some(list) = array.as_any().downcast_ref::<FixedSizeListArray>() {
            if usize::try_from(list.value_length()).ok()? != width { return None; }
            (list.values().as_ref(), None)
        } else if let Some(list) = array.as_any().downcast_ref::<ListArray>() {
            (list.values().as_ref(), Some(list.value_offsets()))
        } else {
            return None;
        };
        let bounds = |row| match rows {
            None => Some((row * width, row * width + width)),
            Some(offsets) => Some((usize::try_from(offsets[row]).ok()?, usize::try_from(offsets[row + 1]).ok()?)),
        };
        let list = array.as_ref();
        let output = match children.data_type() {
            DataType::Float32 => score_rows::<Float32Type>(list, children, &self.query, bounds)?,
            DataType::Float64 => score_rows::<Float64Type>(list, children, &self.query, bounds)?,
            DataType::Union(..) if rtdb_types::ListValue::supports_float_union(children.data_type()) => {
                let view = rtdb_types::ListView::Union(children.as_any().downcast_ref::<UnionArray>()?);
                let mut scores = Float64Builder::with_capacity(list.len());
                for row in 0..list.len() {
                    if list.is_null(row) {
                        scores.append_null();
                        continue;
                    }
                    let (start, end) = bounds(row)?;
                    if (start..end).any(|component| view.float_component(component).is_none()) { return None; }
                    scores.append_value(self.query.score((start..end).map(|component| {
                        view.float_component(component).expect("validated components")
                    }))?);
                }
                scores.finish()
            }
            _ => return None,
        };
        Some(Column::Arrow { array: Arc::new(output), field: Arc::new(Field::new("cosine_distance", DataType::Float64, true)),
            logical: Some(LogicalType::Scalar(ScalarType::F64).maybe()) })
    }
}

fn score_rows<T: ArrowPrimitiveType>(list: &dyn Array, children: &dyn Array, query: &CosineVector,
    bounds: impl Fn(usize) -> Option<(usize, usize)>) -> Option<Float64Array>
where T::Native: Into<f64>,
{
    let values = children.as_any().downcast_ref::<PrimitiveArray<T>>()?;
    let mut builder = Float64Builder::with_capacity(list.len());
    let nulls = values.nulls();
    for row in 0..list.len() {
        if list.is_null(row) {
            builder.append_null();
            continue;
        }
        let (start, end) = bounds(row)?;
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
        let item = Arc::new(Field::new("item", DataType::Float32, false));
        let vector = FixedSizeListArray::try_new(item, 2, Arc::new(arrow_array::Float32Array::from(vec![1.0, 0.0])), None).unwrap();
        let arguments = arrow_array::RecordBatch::try_new(Arc::new(arrow_schema::Schema::new(vec![
            Field::new("?1", vector.data_type().clone(), false),
        ])), vec![Arc::new(vector)]).unwrap();
        let ty = rtdb_columnar::from_field(arguments.schema_ref().field(0)).unwrap();
        let arguments = rtdb_columnar::OwnedBatch::new(arguments, &memory).unwrap();
        let parameters = crate::parameters::BoundParameters::new(vec![Value::Null], vec![ty], &memory).unwrap()
            .with_arrow(Some(crate::parameters::PackedVectors { batch: arguments, mask: vec![true] }));
        let projection = Projection::bind(&expr, &schema, &parameters, &memory).unwrap().unwrap();
        let Some(Column::Arrow { array, .. }) = projection.column(&batch) else { panic!("Expected Arrow distances") };
        let distance = array.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(distance.iter().collect::<Vec<_>>(), vec![Some(0.0), None, Some(1.0)]);
    }
}
